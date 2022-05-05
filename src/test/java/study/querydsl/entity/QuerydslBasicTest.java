package study.querydsl.entity;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.Projections;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import study.querydsl.Dto.MemberDto;
import study.querydsl.Dto.QMemberDto;
import study.querydsl.Dto.UserDto;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.transaction.Transactional;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static study.querydsl.entity.QMember.*;
import static study.querydsl.entity.QTeam.team;

@SpringBootTest
@Transactional
@Rollback(value = false)
public class QuerydslBasicTest {

    @PersistenceContext
    EntityManager em;

    JPAQueryFactory jpaQueryFactory;

    @BeforeEach
    public void before() {
        jpaQueryFactory = new JPAQueryFactory(em);
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);

        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);
        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);

        em.flush();
        em.clear();

//        List<Member> members = em.createQuery("select m from Member m join Fetch m.team", Member.class)
//                .getResultList();
//        for (Member member : members) {
//            System.out.println("member = " + member);
//            System.out.println("member = " + member.getTeam());
//        }
    }

    @Test
    public void startJPQL() throws Exception {
        //member1를 찾아라
        Member findMem1 = em.createQuery("select m from Member m where m.username =:username", Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        assertThat(findMem1.getUsername()).isEqualTo("member1");

    }

    @Test
    public void startQuerydsl() throws Exception {
        Member findMem = jpaQueryFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        assertThat(findMem.getUsername()).isEqualTo("member1");
    }

    @Test
    public void searchAndParam() throws Exception {
        Member findMember = jpaQueryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"),
                        (member.age.eq(10))
                )
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void resultFetch() throws Exception {
        List<Member> fetch = jpaQueryFactory
                .selectFrom(member)
                .fetch();
//        Member fetchOne = jpaQueryFactory
//                .selectFrom(member)
//                .fetchOne();
        Member fetchFirst = jpaQueryFactory
                .selectFrom(member)
                .fetchFirst();
        QueryResults<Member> results = jpaQueryFactory
                .selectFrom(member)
                .fetchResults();

        results.getTotal();
        List<Member> contents = results.getResults();
    }

    /**
     * 1. 회원 나이 내림차순(desc)
     * 2. 회원 이름 올림차순(asc)
     * 단 2에서 회원 이름이 없으면 마지막에 출력(nulls last)
     * @throws Exception
     */
    @Test
    public void sort() throws Exception {
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        List<Member> result = jpaQueryFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();

        Member m5 = result.get(0);
        Member m6 = result.get(1);
        Member m_null = result.get(2);

        Assertions.assertThat(m5.getUsername()).isEqualTo("member5");
        Assertions.assertThat(m6.getUsername()).isEqualTo("member6");
        Assertions.assertThat(m_null.getUsername()).isNull();

    }

    @Test
    public void paging() throws Exception {
        QueryResults<Member> queryResults = jpaQueryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetchResults();

        Assertions.assertThat(queryResults.getTotal()).isEqualTo(4);
        Assertions.assertThat(queryResults.getLimit()).isEqualTo(2);
        Assertions.assertThat(queryResults.getOffset()).isEqualTo(1);
        Assertions.assertThat(queryResults.getResults().size()).isEqualTo(2);
    }

    @Test
    public void aggregation() throws Exception {
        List<Tuple> fetch = jpaQueryFactory
                .select(
                        member.age.count(),
                        member.age.sum()
                )
                .from(member)
                .fetch();

        Tuple tuple = fetch.get(0);
        AssertionsForClassTypes.assertThat(tuple.get(member.age.count())).isEqualTo(4);
        System.out.println("age_count = " + member.age.count());
    }

    /**
     * 팀 A에 소속된 모든 회원
     * @throws Exception
     */
    @Test
    public void join() throws Exception {
        List<Member> teamA = jpaQueryFactory
                .selectFrom(member)
                .join(member.team, team)
                .where(team.name.eq("teamA"))
                .fetch();

        for (Member member1 : teamA) {
            System.out.println(member1.getUsername());
        }
    }

    @Test
    public void theta_join() throws Exception {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));

        List<Member> fetch = jpaQueryFactory
                .select(member)
                .from(member, team)
                .where(member.username.eq(team.name))
                .fetch();
        for (Member fetch1 : fetch) {
            System.out.println(fetch1.getUsername());

        }
    }

    /**
     * 회원과 팀을 조인하면서, 팀 이름이 teamA 팀만 조인, 회원은 모두 조회
     * JPQL: select m, t from Member m left outer join m.team t on t.name = 'teamA'
     * @throws Exception
     */
    @Test
    public void join_on_filtering() throws Exception {
        List<Tuple> teamA = jpaQueryFactory
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team).on(team.name.eq("teamA"))
                .fetch();

        for (Tuple tuple : teamA) {
            System.out.println("tuple = " + tuple);
        }
    }

    @Test
    public void join_on_no_relation() throws Exception {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Tuple> fetch = jpaQueryFactory
                .select(member, team)
                .from(member)
                .leftJoin(team).on(member.username.eq(team.name))
                .fetch();
        for (Tuple tuple : fetch) {
            System.out.println("tuple = " + tuple);
        }
    }

    @Test
    public void fetch_join_NO() throws Exception {
        em.flush();
        em.clear();

        Member fetchOne = jpaQueryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();
    }

    @Test
    public void fetch_join() throws Exception {
        em.flush();
        em.clear();

        Member fetchOne = jpaQueryFactory
                .selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();
    }

    /**
     * 나이가 가장 많은 회원 조회
     */
    @Test
    public void subQuery() throws Exception {
        QMember memberSub = new QMember("memberSub");

        List<Member> fetch = jpaQueryFactory
                .selectFrom(member)
                .where(member.age.eq(
                        JPAExpressions
                                .select(memberSub.age.max())
                                .from(memberSub)
                ))
                .fetch();
        assertThat(fetch).extracting("age")
                .containsExactly(40);
    }
    /**
     * 나이가 평균 이상인 회원 조회
     */
    @Test
    public void subQueryGoe() throws Exception {
        QMember memberSub = new QMember("memberSub");

        List<Member> fetch = jpaQueryFactory
                .selectFrom(member)
                .where(member.age.goe(
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();
        assertThat(fetch).extracting("age")
                .containsExactly(30, 40);
    }

    @Test
    public void select_subQuery() throws Exception {
        QMember memberSub = new QMember("memberSub");

        List<Tuple> fetch = jpaQueryFactory
                .select(member.username,
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub))
                .from(member)
                .fetch();
        for (Tuple tuple : fetch) {
            System.out.println("tuple = " + tuple);
        }
    }

    @Test
    public void basicCode() throws Exception {
        List<String> fetch = jpaQueryFactory
                .select(member.age
                        .when(10).then("열살")
                        .when(20).then("스무살")
                        .otherwise("기타"))
                .from(member)
                .fetch();
        for (String s : fetch) {
            System.out.println("s = " + s);
        }
    }

    @Test
    public void simpleProjection() throws Exception {
        jpaQueryFactory
                .select(member.username, member.age, member.id)
                .from(member)
                .fetch();
    }

    @Test
    public void findDtoBySetter() throws Exception {
        List<MemberDto> result = jpaQueryFactory
                .select(Projections.fields(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();
        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    @Test
    public void findUserDtoBySetter() throws Exception {
        QMember memberSub = new QMember("memberSub");
        List<UserDto> result = jpaQueryFactory
                .select(Projections.constructor(UserDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();
        for (UserDto userDto : result) {
            System.out.println(userDto);
        }
    }

    @Test
    public void findDtoByQueryProjection() throws Exception {
        List<MemberDto> result = jpaQueryFactory
                .select(new QMemberDto(member.username, member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    @Test
    public void dynamicQuery_BooleanBuilder() throws Exception {
        String usernameParam = "member1";
        Integer ageParam = 10;

        List<Member> result = searchMember1(usernameParam, ageParam);
        assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMember1(String usernameCond, Integer ageCond) {
        BooleanBuilder builder = new BooleanBuilder();
        if (usernameCond != null) {
            builder.and(member.username.eq(usernameCond));
        }
        if (ageCond != null) {
            builder.and(member.age.eq(ageCond));
        }
        return jpaQueryFactory
                .selectFrom(member)
                .where(builder)
                .fetch();
    }
}
