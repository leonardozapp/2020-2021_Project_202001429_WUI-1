<?php
declare(strict_types=1);

namespace App\Model\Table;

use Cake\ORM\Query;
use Cake\ORM\RulesChecker;
use Cake\ORM\Table;
use Cake\Validation\Validator;

/**
 * UsuariosAguardandoValidacoesEmails Model
 *
 * @method \App\Model\Entity\UsuariosAguardandoValidacoesEmail newEmptyEntity()
 * @method \App\Model\Entity\UsuariosAguardandoValidacoesEmail newEntity(array $data, array $options = [])
 * @method \App\Model\Entity\UsuariosAguardandoValidacoesEmail[] newEntities(array $data, array $options = [])
 * @method \App\Model\Entity\UsuariosAguardandoValidacoesEmail get($primaryKey, $options = [])
 * @method \App\Model\Entity\UsuariosAguardandoValidacoesEmail findOrCreate($search, ?callable $callback = null, $options = [])
 * @method \App\Model\Entity\UsuariosAguardandoValidacoesEmail patchEntity(\Cake\Datasource\EntityInterface $entity, array $data, array $options = [])
 * @method \App\Model\Entity\UsuariosAguardandoValidacoesEmail[] patchEntities(iterable $entities, array $data, array $options = [])
 * @method \App\Model\Entity\UsuariosAguardandoValidacoesEmail|false save(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\UsuariosAguardandoValidacoesEmail saveOrFail(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\UsuariosAguardandoValidacoesEmail[]|\Cake\Datasource\ResultSetInterface|false saveMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\UsuariosAguardandoValidacoesEmail[]|\Cake\Datasource\ResultSetInterface saveManyOrFail(iterable $entities, $options = [])
 * @method \App\Model\Entity\UsuariosAguardandoValidacoesEmail[]|\Cake\Datasource\ResultSetInterface|false deleteMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\UsuariosAguardandoValidacoesEmail[]|\Cake\Datasource\ResultSetInterface deleteManyOrFail(iterable $entities, $options = [])
 */
class UsuariosAguardandoValidacoesEmailsTable extends Table
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

        $this->setTable('usuarios_aguardando_validacoes_emails');
        $this->setDisplayField('usuarios_id');
        $this->setPrimaryKey('usuarios_id');
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
            ->integer('usuarios_id')
            ->allowEmptyString('usuarios_id', null, 'create');

        $validator
            ->scalar('codigo_validacao')
            ->maxLength('codigo_validacao', 32)
            ->requirePresence('codigo_validacao', 'create')
            ->notEmptyString('codigo_validacao');

        $validator
            ->dateTime('data_envio_email')
            ->requirePresence('data_envio_email', 'create')
            ->notEmptyDateTime('data_envio_email');

        return $validator;
    }
}
