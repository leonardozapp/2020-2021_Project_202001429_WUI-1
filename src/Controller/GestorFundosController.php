<?php
declare(strict_types=1);

namespace App\Controller;

/**
 * GestorFundos Controller
 *
 * @property \App\Model\Table\GestorFundosTable $GestorFundos
 * @method \App\Model\Entity\GestorFundo[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class GestorFundosController extends AppController
{
    /**
     * Index method
     *
     * @return \Cake\Http\Response|null|void Renders view
     */
    public function index()
    {
        $gestorFundos = $this->paginate($this->GestorFundos);

        $this->set(compact('gestorFundos'));
    }

    /**
     * View method
     *
     * @param string|null $id Gestor Fundo id.
     * @return \Cake\Http\Response|null|void Renders view
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function view($id = null)
    {
        $gestorFundo = $this->GestorFundos->get($id, [
            'contain' => ['CadastroFundos'],
        ]);

        $this->set(compact('gestorFundo'));
    }

    /**
     * Add method
     *
     * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
     */
    public function add()
    {
        $gestorFundo = $this->GestorFundos->newEmptyEntity();
        if ($this->request->is('post')) {
            $gestorFundo = $this->GestorFundos->patchEntity($gestorFundo, $this->request->getData());
            if ($this->GestorFundos->save($gestorFundo)) {
                $this->Flash->success(__('The gestor fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The gestor fundo could not be saved. Please, try again.'));
        }
        $this->set(compact('gestorFundo'));
    }

    /**
     * Edit method
     *
     * @param string|null $id Gestor Fundo id.
     * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function edit($id = null)
    {
        $gestorFundo = $this->GestorFundos->get($id, [
            'contain' => [],
        ]);
        if ($this->request->is(['patch', 'post', 'put'])) {
            $gestorFundo = $this->GestorFundos->patchEntity($gestorFundo, $this->request->getData());
            if ($this->GestorFundos->save($gestorFundo)) {
                $this->Flash->success(__('The gestor fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The gestor fundo could not be saved. Please, try again.'));
        }
        $this->set(compact('gestorFundo'));
    }

    /**
     * Delete method
     *
     * @param string|null $id Gestor Fundo id.
     * @return \Cake\Http\Response|null|void Redirects to index.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function delete($id = null)
    {
        $this->request->allowMethod(['post', 'delete']);
        $gestorFundo = $this->GestorFundos->get($id);
        if ($this->GestorFundos->delete($gestorFundo)) {
            $this->Flash->success(__('The gestor fundo has been deleted.'));
        } else {
            $this->Flash->error(__('The gestor fundo could not be deleted. Please, try again.'));
        }

        return $this->redirect(['action' => 'index']);
    }
}
