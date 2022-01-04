<?php

declare(strict_types=1);

namespace App\Model\Table;

use Cake\ORM\Query;
use Cake\ORM\RulesChecker;
use Cake\ORM\Table;
use Cake\Validation\Validator;

/**
 * Usuarios Model
 *
 * @property \App\Model\Table\TipoPlanosTable&\Cake\ORM\Association\BelongsTo $TipoPlanos
 * @property \App\Model\Table\TipoEtapasRegistrosTable&\Cake\ORM\Association\BelongsTo $TipoEtapasRegistros
 * @property \App\Model\Table\CarteirasInvestimentosTable&\Cake\ORM\Association\HasMany $CarteirasInvestimentos
 * @property \App\Model\Table\TipoPerfilInvestidorTable&\Cake\ORM\Association\BelongsTo $TipoPerfilInvestidor
 *
 * @method \App\Model\Entity\Usuario newEmptyEntity()
 * @method \App\Model\Entity\Usuario newEntity(array $data, array $options = [])
 * @method \App\Model\Entity\Usuario[] newEntities(array $data, array $options = [])
 * @method \App\Model\Entity\Usuario get($primaryKey, $options = [])
 * @method \App\Model\Entity\Usuario findOrCreate($search, ?callable $callback = null, $options = [])
 * @method \App\Model\Entity\Usuario patchEntity(\Cake\Datasource\EntityInterface $entity, array $data, array $options = [])
 * @method \App\Model\Entity\Usuario[] patchEntities(iterable $entities, array $data, array $options = [])
 * @method \App\Model\Entity\Usuario|false save(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\Usuario saveOrFail(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\Usuario[]|\Cake\Datasource\ResultSetInterface|false saveMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\Usuario[]|\Cake\Datasource\ResultSetInterface saveManyOrFail(iterable $entities, $options = [])
 * @method \App\Model\Entity\Usuario[]|\Cake\Datasource\ResultSetInterface|false deleteMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\Usuario[]|\Cake\Datasource\ResultSetInterface deleteManyOrFail(iterable $entities, $options = [])
 *
 * @mixin \Cake\ORM\Behavior\TimestampBehavior
 */
class UsuariosTable extends Table
{
    /**
     * Initialize method
     *
     * @param array $config The configuration for the Table.
     * @return void
     */
    public function initialize(array $config): void
    {
        parent::initialize($config);

        $this->setTable('usuarios');
        $this->setDisplayField('nome');
        $this->setPrimaryKey('id');

        $this->addBehavior('Timestamp');

        $this->belongsTo('TipoPlanos', [
            'foreignKey' => 'tipo_plano_id',
            'joinType' => 'INNER',
        ]);
        $this->belongsTo('TipoEtapasRegistros', [
            'foreignKey' => 'tipo_etapas_registro_id',
            'joinType' => 'INNER',
        ]);
        $this->hasMany('CarteirasInvestimentos', [
            'foreignKey' => 'usuario_id',
        ]);

        $this->belongsTo('TipoPerfilInvestidor', [
            'foreignKey' => 'tipo_perfil_investidor_id',
            'joinType' => 'LEFT',
        ]);
    }

    /**
     * Default validation rules.
     *
     * @param \Cake\Validation\Validator $validator Validator instance.
     * @return \Cake\Validation\Validator
     */
    public function validationDefault(Validator $validator): Validator
    {
        $validator
            ->integer('id')
            ->allowEmptyString('id', null, 'create');

        $validator
            ->scalar('cpf')
            ->maxLength('cpf', 11)
            ->requirePresence('cpf', 'create')
            ->notEmptyString('cpf')
            ->add('cpf', 'unique', ['rule' => 'validateUnique', 'provider' => 'table']);

        $validator
            ->scalar('nome')
            ->maxLength('nome', 200)
            ->requirePresence('nome', 'create')
            ->notEmptyString('nome');

        $validator
            ->email('email')
            ->requirePresence('email', 'create')
            ->notEmptyString('email');

        $validator
            ->date('dt_nasc')
            ->requirePresence('dt_nasc', 'create')
            ->notEmptyDate('dt_nasc');

        $validator
            ->scalar('senha')
            ->maxLength('senha', 64)
            ->requirePresence('senha', 'create')
            ->notEmptyString('senha');

        $validator
            ->date('dt_reg')
            ->requirePresence('dt_reg', 'create')
            ->notEmptyDate('dt_reg');

        $validator
            ->scalar('coment')
            ->maxLength('coment', 100)
            ->allowEmptyString('coment');

        return $validator;
    }

    /**
     * Returns a rules checker object that will be used for validating
     * application integrity.
     *
     * @param \Cake\ORM\RulesChecker $rules The rules object to be modified.
     * @return \Cake\ORM\RulesChecker
     */
    public function buildRules(RulesChecker $rules): RulesChecker
    {
        $rules->add($rules->isUnique(['cpf']), ['errorField' => 'cpf']);
        $rules->add($rules->existsIn(['tipo_plano_id'], 'TipoPlanos'), ['errorField' => 'tipo_plano_id']);
        $rules->add($rules->existsIn(['tipo_etapas_registro_id'], 'TipoEtapasRegistros'), ['errorField' => 'tipo_etapas_registro_id']);
        $rules->add($rules->existsIn(['tipo_perfil_investidor_id'], 'TipoPerfilInvestidor'), ['errorField' => 'tipo_perfil_investidor_id']);

        return $rules;
    }
}
